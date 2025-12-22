import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

export interface UsersInterface extends Model {
    id: number;
    username: string;
    password: string;
    email: string;
    identical: string;
    ack_code: string;
    created_at: Date;
    updated_at: Date;
}

export const Users = sequelize.define<UsersInterface>(
    'Users',
    {
        id: {
            type: DataTypes.BIGINT({ length: 20 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        username: DataTypes.STRING,
        password: DataTypes.STRING,
        email: DataTypes.STRING,
        identical: DataTypes.STRING,
        ack_code: DataTypes.STRING,
        created_at: DataTypes.DATE,
        updated_at: DataTypes.DATE,
    },
    {
        tableName: 'users',
        underscored: true,
    },
);
